#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Who may see and do what in the web console: the access model (permissions, roles, teams, pipe
access and locks) applied to the person looking at a page.

The rules are `meerschaum.core.access`'s, the same as the API server's (`meerschaum_core::access`).
The **viewer** of a request (the console session's user, with their effective permissions and
teams) is `meerschaum.api.dash.viewer`'s; this module asks the rules about the pipes, Pipelines,
runs, jobs and users a page would show. Every page and callback goes through it, and every check
runs on the server side of the callback: a page hides what the viewer may not do, and the callback
refuses it again when asked anyway.

- `resolve_viewer(session)` / `current_viewer()`: who is asking (`meerschaum.api.dash.viewer`).
- `can_see_pipe`, `can_edit_pipe`, `can_manage_access`, `can_lock`: a pipe (or its parameters),
  for a viewer.
- `visible_refs(viewer, instance)`: the `pipe_ref`s a viewer may see on an instance (one read of
  the registered pipes), for filtering runs, statuses, graphs and counts.
- `can_see_email(viewer, username)`: admins, and managers of a team the user is in.

Under `--no-auth` the viewer is an administrator. Locally (the CLI, Python) nothing here applies:
the instance's credentials are the trust boundary; locks are enforced by the core everywhere.
"""

from __future__ import annotations

import threading
import time

import meerschaum as mrsm
from typing import Set
from meerschaum.utils.typing import Any, Dict, Iterable, List, Optional, Tuple, SuccessTuple
from meerschaum.api.dash.viewer import (
    ANONYMOUS,
    EVERYONE,
    PERMISSION_GROUPS,
    ROLE_LABELS,
    SYSTEM,
    Viewer,
    all_permissions,
    core,
    current_viewer,
    default_role,
    forget,
    make_viewer,
    register_invalidation_hook,
    resolve_viewer,
    role_name,
    role_permissions,
    viewer_for,
)

__all__ = (
    # The viewer (`meerschaum.api.dash.viewer`).
    'ANONYMOUS', 'EVERYONE', 'PERMISSION_GROUPS', 'ROLE_LABELS', 'SYSTEM', 'Viewer',
    'all_permissions', 'core', 'current_viewer', 'default_role', 'forget', 'make_viewer',
    'resolve_viewer', 'role_permissions', 'viewer_for',
    # Pipes and their locks.
    'EDIT_MODES', 'HIDDEN_LABEL', 'LOCKS', 'PIPE_ACTION_LOCK_OPS', 'PIPE_ACTION_PERMISSIONS',
    'REFS_TTL_SECONDS', 'VISIBILITIES', 'access_fields', 'assignable_teams', 'can_edit_pipe',
    'can_lock', 'can_manage_access', 'can_see_pipe', 'check_parameters_change', 'hidden_label',
    'lock_refusal', 'may_run_pipe_action', 'new_pipe_access', 'partition_refs', 'pipe_access',
    'pipe_locks', 'pipe_ref', 'ref_is_visible', 'registered_pipes', 'resolve_visible_pipe',
    'sql_definition_changed', 'visible_pipe', 'visible_pipe_from_meta', 'visible_pipes',
    'visible_ref_subset', 'visible_refs',
    # Pipelines.
    'hidden_members', 'may_sync_pipeline', 'pipeline_registry', 'pipeline_registry_parameters',
    'visible_pipeline', 'visible_pipelines',
    # Users' rights and jobs.
    'attributes_with_rights', 'can_see_email', 'filter_jobs', 'user_rights',
)

#: How long (seconds) an instance's visible pipes are reused.
REFS_TTL_SECONDS: float = 5.0

#: The permission each pipe action needs (the same as its HTTP route's scope).
PIPE_ACTION_PERMISSIONS: Dict[str, str] = {
    'sync': 'pipes:write',
    'verify': 'pipes:write',
    'clear': 'pipes:delete',
    'drop': 'pipes:drop',
    'delete': 'pipes:delete',
    'copy': 'pipes:register',
    'register': 'pipes:register',
    'edit': 'pipes:write',
}

#: The write each pipe action is (`meerschaum.core.access.LOCK_OPS`), which its locks refuse.
PIPE_ACTION_LOCK_OPS: Dict[str, str] = {
    'sync': 'sync',
    'verify': 'sync',
    'clear': 'clear',
    'drop': 'drop',
    'delete': 'delete_and_drop',
    'edit': 'edit',
}

#: The locks, in the order the Locks dialog lists them: `(lock, label)`.
LOCKS: Tuple[Tuple[str, str], ...] = (
    ('parameters', 'Edit parameters'),
    ('sync', 'Sync'),
    ('clear', 'Clear rows'),
    ('drop', 'Drop or rebuild'),
    ('delete', 'Delete'),
)

#: Visibility and edit modes: `(value, label)`.
VISIBILITIES: Tuple[Tuple[str, str], ...] = (
    ('team', 'Team'),
    ('private', 'Private'),
    ('everyone', 'Everyone'),
)
EDIT_MODES: Tuple[Tuple[str, str], ...] = (
    ('team', 'Anyone who can see it'),
    ('owner', 'Owner and team managers'),
)


# ------------------------------------------------------------------------------------------------
# Pipes
# ------------------------------------------------------------------------------------------------

def _parameters_of(pipe_or_parameters: Any) -> Dict[str, Any]:
    if isinstance(pipe_or_parameters, dict):
        return pipe_or_parameters
    if pipe_or_parameters is None:
        raise ValueError('The pipe is not available.')
    getter = getattr(pipe_or_parameters, 'get_parameters', None)
    if callable(getter):
        return getter(apply_symlinks=False) or {}
    return dict(pipe_or_parameters.parameters or {})


def pipe_access(pipe_or_parameters: Any) -> Dict[str, Any]:
    """The pipe's `access` (`{owner, team, visibility, edit}`, the defaults for missing keys)."""
    return dict(core().get_pipe_access(_parameters_of(pipe_or_parameters)))


def access_fields(pipe_or_parameters: Any) -> Dict[str, Optional[str]]:
    """`{owner, team, visibility, edit}` as plain strings (`team` is `None` for Everyone)."""
    value = pipe_access(pipe_or_parameters)
    team = value.get('team', None)
    return {
        'owner': (str(value['owner']) if value.get('owner') else None),
        'team': (None if core().is_everyone(team) else str(team)),
        'visibility': str(value.get('visibility') or 'team').lower(),
        'edit': str(value.get('edit') or 'team').lower(),
    }


def pipe_locks(pipe_or_parameters: Any) -> List[str]:
    """The pipe's locks (`all` expanded), in the Locks dialog's order."""
    names = {str(name).lower() for name in core().get_locks(_parameters_of(pipe_or_parameters))}
    if 'all' in names:
        names.update(lock for lock, _ in LOCKS)
    return [lock for lock, _ in LOCKS if lock in names]


def _rule(name: str, viewer: Viewer, pipe_or_parameters: Any) -> bool:
    if viewer.no_auth or (viewer.signed_in and viewer.is_admin):
        return True
    if not viewer.signed_in:
        return False
    try:
        parameters = pipe_access(pipe_or_parameters)
    except Exception:
        ### An unreadable registration must never become an unrestricted Everyone pipe,
        ### even when a stale object still has a cached parameters dictionary.
        return False
    func = getattr(core(), name)
    return bool(func(viewer.principal, parameters))


def can_see_pipe(viewer: Viewer, pipe_or_parameters: Any) -> bool:
    """Whether the viewer may see the pipe (and it needs `pipes:read`)."""
    if not viewer.can('pipes:read'):
        return False
    if viewer.no_auth or viewer.is_admin:
        return _rule('can_see', viewer, pipe_or_parameters)
    try:
        parameters = _parameters_of(pipe_or_parameters)
    except Exception:
        return False
    if not _rule('can_see', viewer, parameters):
        return False
    from meerschaum.api.dash.sql_guard import TARGET_KEYS, check_target
    return not any(
        check_target(str(parameters[key]), ())
        for key in TARGET_KEYS if parameters.get(key)
    )


def can_edit_pipe(viewer: Viewer, pipe_or_parameters: Any, permission: str = 'pipes:write') -> bool:
    """Whether the viewer may change the pipe (within `permission`)."""
    if not viewer.can(permission) or not can_see_pipe(viewer, pipe_or_parameters):
        return False
    return _rule('can_edit', viewer, pipe_or_parameters)


def can_manage_access(viewer: Viewer, pipe_or_parameters: Any) -> bool:
    """Whether the viewer may change the pipe's access (its owner, team managers, admins)."""
    if not can_see_pipe(viewer, pipe_or_parameters):
        return False
    return _rule('can_manage_access', viewer, pipe_or_parameters)


def can_lock(viewer: Viewer, pipe_or_parameters: Any) -> bool:
    """Whether the viewer may lock and unlock the pipe (its owner, team managers, admins)."""
    if not can_see_pipe(viewer, pipe_or_parameters):
        return False
    return _rule('can_lock', viewer, pipe_or_parameters)


def may_run_pipe_action(viewer: Viewer, pipe_or_parameters: Any, action: str) -> SuccessTuple:
    """
    `(allowed, reason)`: whether the viewer may run a pipe action (`sync`, `verify`, `clear`,
    `drop`, `delete`, `copy`, `edit`): its permission, edit rights on the pipe, and its lock.
    """
    permission = PIPE_ACTION_PERMISSIONS.get(action, 'pipes:write')
    if not can_see_pipe(viewer, pipe_or_parameters):
        return False, "This pipe is not available."
    if not viewer.can(permission):
        return False, f"Your account may not {action} pipes (`{permission}`)."
    if action != 'copy' and not can_edit_pipe(viewer, pipe_or_parameters, permission):
        return False, "Only its owner and its team's managers may change this pipe."
    refusal = lock_refusal(pipe_or_parameters, action)
    if refusal:
        return False, refusal
    return True, ''


def lock_refusal(pipe_or_parameters: Any, action: str, pipe: Any = None) -> Optional[str]:
    """
    Why a locked pipe refuses `action` (the core's message: the pipe, the lock and how to unlock
    it), or `None`.
    """
    op = PIPE_ACTION_LOCK_OPS.get(action, None)
    if op is None:
        return None
    pipe = pipe if pipe is not None else (pipe_or_parameters if not isinstance(pipe_or_parameters, dict) else None)
    locks = core().get_locks(_parameters_of(pipe_or_parameters))
    if not locks:
        return None
    if pipe is not None:
        ### The core's message names the pipe, the lock and how to unlock it (Manage > Locks).
        message = core().lock_refusal(pipe, locks, op)
        return str(message) if message else None
    message = core().check_lock(locks, op)
    if message:
        return f"This pipe refuses to {action}: {message}. Unlock it in Manage > Locks."
    return None


def sql_definition_changed(connector_keys: str, old: Dict[str, Any], new: Dict[str, Any]) -> bool:
    """Whether going from `old` to `new` parameters sets or changes the pipe's SQL definition."""
    return bool(core().sql_definition_changed(str(connector_keys), dict(old or {}), dict(new or {})))


def check_parameters_change(
    viewer: Viewer,
    pipe: Any,
    old: Dict[str, Any],
    new: Dict[str, Any],
) -> SuccessTuple:
    """
    Whether the viewer may save `new` parameters over `old` from the console: edit rights; a
    changed `access` needs the right to manage it (and the core's rule for the change); changed
    `locks` need the right to lock; a changed SQL definition needs `pipes:define_sql`; and under
    a `parameters` lock only `locks` may change (`lock_refusal`).
    """
    if viewer.no_auth:
        return True, ''
    if not can_edit_pipe(viewer, old, 'pipes:write'):
        return False, "Only its owner and its team's managers may change this pipe."
    old = dict(old or {})
    new = dict(new or {})
    if (old.get('access') or None) != (new.get('access') or None):
        if not can_manage_access(viewer, old):
            return False, "Only the pipe's owner, its team's managers and administrators may change its access."
        try:
            refusal = core().check_access_change(viewer.principal, pipe_access(old), pipe_access(new))
        except Exception as e:
            return False, str(e)
        if refusal:
            return False, str(refusal)
    if (old.get('locks') or None) != (new.get('locks') or None) and not can_lock(viewer, old):
        return False, "Only the pipe's owner, its team's managers and administrators may lock or unlock it."
    connector_keys = str(getattr(pipe, 'connector_keys', '') or '')
    if sql_definition_changed(connector_keys, old, new) and not viewer.can('pipes:define_sql'):
        return False, "Your account may not set SQL definitions."
    other_keys = {key for key in set(old) | set(new) if key not in ('locks',)}
    if any(old.get(key) != new.get(key) for key in other_keys):
        refusal = lock_refusal(old, 'edit', pipe=(pipe if hasattr(pipe, 'connector_keys') else None))
        if refusal:
            return False, refusal
    return True, ''


def visible_pipes(viewer: Viewer, pipes: Iterable[Any]) -> List[Any]:
    """The pipes the viewer may see (their parameters as listed; no new reads)."""
    return [pipe for pipe in pipes if can_see_pipe(viewer, pipe.parameters if hasattr(pipe, 'parameters') else pipe)]


_refs_cache: Dict[Tuple[str, str], Tuple[float, Optional[Set[str]]]] = {}
_refs_lock = threading.Lock()


def _forget_refs(username: Optional[str]) -> None:
    """Drop every cached visible-pipes set (a change of anyone's rights may change them)."""
    with _refs_lock:
        _refs_cache.clear()


register_invalidation_hook(_forget_refs)


def pipe_ref(instance_keys: str, connector_keys: Any, metric_key: Any, location_key: Any) -> str:
    """A pipe's `pipe_ref` without its instance's keys parsed (`instance|ck|mk|lk`)."""
    return '|'.join((str(instance_keys), str(connector_keys), str(metric_key), str(location_key)))


def registered_pipes(
    instance: Any,
    **filters: Any
) -> List[Tuple[str, str, Optional[str], Dict[str, Any]]]:
    """
    `[(connector_keys, metric_key, location_key, parameters)]` of the pipes registered on
    `instance` (one `fetch_pipes_keys`; internal `mrsm` pipes only with `connector_keys=['mrsm']`).
    An instance that lists keys without parameters (a custom instance connector may) has each
    pipe's parameters read, so its `access` is never guessed.
    """
    import json
    from meerschaum.utils import fetch_pipes_keys
    conn = instance if not isinstance(instance, str) else mrsm.get_connector(instance)
    result = fetch_pipes_keys('registered', conn, **filters)
    hydrated = isinstance(result, dict)
    items = list(result.values()) if hydrated else list(result or [])
    out = []
    for keys_tuple in items:
        if not isinstance(keys_tuple, (list, tuple)) or len(keys_tuple) < 3:
            continue
        ck, mk, lk = keys_tuple[0], keys_tuple[1], keys_tuple[2]
        lk = None if lk in (None, 'None') else lk
        parameters = keys_tuple[3] if len(keys_tuple) > 3 else None
        if isinstance(parameters, str):
            try:
                parameters = json.loads(parameters)
            except ValueError:
                parameters = None
        # List results (Valkey and custom instances) carry key extras, not an ACL snapshot.
        # In particular their empty fourth dictionary must never imply public access.
        if not hydrated or not isinstance(parameters, dict):
            try:
                pipe = mrsm.Pipe(ck, mk, lk, instance=conn)
                failures = pipe.__dict__.get('_attributes_load_failures', 0)
                parameters = pipe.get_parameters(apply_symlinks=False, refresh=True)
                if (not isinstance(parameters, dict)
                    or pipe.__dict__.get('_attributes_load_failures', 0) > failures):
                    raise ValueError('Pipe metadata is unavailable.')
                if not parameters and not pipe.attributes.get('pipe_id') and pipe.get_id() is None:
                    raise ValueError('Pipe registration is unavailable.')
            except Exception:
                ### Unreadable: treated as private to nobody but administrators.
                parameters = {'access': {'visibility': 'private', 'owner': None}}
        out.append((str(ck), str(mk), (None if lk is None else str(lk)), parameters))
    return out


def visible_refs(viewer: Viewer, instance: Any) -> Optional[Set[str]]:
    """
    The `pipe_ref`s of the registered pipes the viewer may see on `instance`, or `None` when the
    viewer sees every pipe (administrators, `--no-auth`). One bulk read of the instance's pipes
    (keys and parameters), reused for `REFS_TTL_SECONDS`.
    """
    if viewer.no_auth or viewer.is_admin:
        return None
    if not viewer.can('pipes:read'):
        return set()
    instance_keys = str(instance)
    key = (str(viewer.username), instance_keys)
    now = time.monotonic()
    with _refs_lock:
        entry = _refs_cache.get(key, None)
    if entry is not None and now - entry[0] < REFS_TTL_SECONDS:
        return entry[1]
    refs: Set[str] = set()
    try:
        if isinstance(instance, str):
            from meerschaum.api.dash.engine import get_instance
            instance = get_instance(instance, strict=True)
        pipes = registered_pipes(instance)
    except Exception:
        ### An instance this API does not serve (or cannot read) shows nothing.
        pipes = []
    for ck, mk, lk, parameters in pipes:
        if can_see_pipe(viewer, parameters):
            refs.add(pipe_ref(instance_keys, ck, mk, lk))
    with _refs_lock:
        if len(_refs_cache) > 2048:
            _refs_cache.clear()
        _refs_cache[key] = (now, refs)
    return refs


def visible_ref_subset(viewer: Viewer, refs: Iterable[str]) -> Set[str]:
    """Fresh, bounded registration reads for referenced pipes; no cached access results."""
    requested = {str(ref) for ref in refs}
    if viewer.no_auth or viewer.is_admin:
        return requested
    if not viewer.signed_in or not viewer.can('pipes:read'):
        return set()
    from meerschaum.api.dash.engine import get_instance
    groups: Dict[str, List[Tuple[str, str, str]]] = {}
    for ref in requested:
        parts = ref.split('|')
        if len(parts) == 4 and parts[1] != 'mrsm':
            groups.setdefault(parts[0], []).append(tuple(parts[1:]))
    visible = set()
    for keys, members in groups.items():
        instance_visible = set()
        try:
            conn = get_instance(keys, strict=True)
            for offset in range(0, len(members), 128):
                chunk = members[offset:offset + 128]
                wanted = set(chunk)
                rows = registered_pipes(conn,
                    connector_keys=sorted({member[0] for member in chunk}),
                    metric_keys=sorted({member[1] for member in chunk}),
                )
                instance_visible.update(
                    pipe_ref(keys, ck, mk, lk) for ck, mk, lk, parameters in rows
                    if (str(ck), str(mk), str(lk)) in wanted and can_see_pipe(viewer, parameters)
                )
            visible.update(instance_visible)
        except Exception:
            continue  # Refused/unreadable instances supply no visible references.
    return visible


def pipeline_registry(instance: Any) -> List[Tuple[str, Dict[str, Any]]]:
    """`[(name, registry_parameters)]` of the saved Pipelines of `instance` (one read)."""
    out = []
    for _, _, name, parameters in registered_pipes(instance, connector_keys=['mrsm'], metric_keys=['pipeline']):
        if name and isinstance(parameters.get('pipeline'), dict):
            out.append((str(name), parameters))
    return out


def visible_pipelines(
    instance: Any,
    viewer: Optional[Viewer] = None,
    definitions: Optional[Dict[str, Dict[str, Any]]] = None,
    include_members: bool = True,
    registry_parameters: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    The saved Pipelines of `instance` that `viewer` may see (their registry pipe's `access`),
    as `list_pipelines()` reads them (or the `definitions` a caller already read). A
    Pipeline's members the viewer may not see are left out of its `targets` and counted
    (`hidden_members`). `include_members=False` omits members entirely for list cards, so
    listing saved names does not walk their member instances.
    """
    from meerschaum.core.Pipeline import _saved
    viewer = viewer if viewer is not None else current_viewer()
    conn = instance if not isinstance(instance, str) else mrsm.get_connector(instance)
    if viewer.no_auth or viewer.is_admin:
        return dict(definitions if definitions is not None else _saved.list_pipelines(conn))
    ### The definitions as every other reader sees them (`list_pipelines`), filtered by their
    ### registry pipes' `access`.
    try:
        registry = dict(registry_parameters) if registry_parameters is not None else dict(pipeline_registry(conn))
    except Exception:
        if viewer.no_auth or viewer.is_admin:
            raise
        return {}
    definitions = dict(definitions) if definitions is not None else {
        name: _saved._normalize(parameters['pipeline'], name, conn)
        for name, parameters in registry.items()
    }
    if not viewer.signed_in or not viewer.can('pipes:read'):
        return {}
    visible_definitions: Dict[str, Dict[str, Any]] = {}
    for name, definition in definitions.items():
        parameters = registry.get(str(name))
        if parameters is None or not can_see_pipe(viewer, parameters):
            continue
        definition = dict(definition)
        targets = [str(ref) for ref in (definition.get('targets') or [])]
        visible, hidden = partition_refs(viewer, targets) if include_members else ([], 0)
        definition['targets'] = visible
        definition['hidden_members'] = hidden
        visible_definitions[name] = definition
    return visible_definitions


def visible_pipeline(name: str, instance: Any, viewer: Optional[Viewer] = None) -> Optional[Dict[str, Any]]:
    """One saved Pipeline as `visible_pipelines()` reads it, or `None`."""
    return visible_pipelines(instance, viewer=viewer).get(str(name))


def pipeline_registry_parameters(name: str, instance: Any) -> Dict[str, Any]:
    """The parameters of a saved Pipeline's registry pipe (`Pipe('mrsm', 'pipeline', name)`)."""
    from meerschaum.core.Pipeline._saved import registration
    try:
        return registration(name, instance).get_parameters(apply_symlinks=False, refresh=True) or {}
    except Exception:
        return {}


def hidden_members(name: str, instance: Any, viewer: Optional[Viewer] = None) -> List[str]:
    """The stored members of a Pipeline `viewer` may not see (kept when they save it)."""
    viewer = viewer if viewer is not None else current_viewer()
    if viewer.no_auth or viewer.is_admin:
        return []
    parameters = pipeline_registry_parameters(name, instance)
    targets = [str(ref) for ref in ((parameters.get('pipeline') or {}).get('targets') or [])]
    return [ref for ref in targets if not ref_is_visible(viewer, ref)]


def may_sync_pipeline(viewer: Viewer, name: str, instance: Any) -> SuccessTuple:
    """
    Whether the viewer may run a saved Pipeline from the console: they may see it, and change
    (`pipes:write`, edit rights) every member it lists, none hidden from them. Locked members are
    the engine's to skip.
    """
    if viewer.no_auth or viewer.is_admin:
        return True, ''
    parameters = pipeline_registry_parameters(name, instance)
    if not parameters or not can_see_pipe(viewer, parameters):
        return False, f"Pipeline '{name}' is not available."
    if not viewer.can('pipes:write'):
        return False, "Your account may not sync pipes."
    targets = [str(ref) for ref in ((parameters.get('pipeline') or {}).get('targets') or [])]
    for ref in targets:
        if not ref_is_visible(viewer, ref):
            return False, "This Pipeline has pipes you cannot see: ask their team to run it."
    from meerschaum.api.dash.engine import split_ref
    for ref in targets:
        instance_keys, ck, mk, lk = split_ref(ref)
        pipe = visible_pipe(ck, mk, lk, instance_keys, viewer=viewer)
        if pipe is None or not can_edit_pipe(viewer, pipe):
            return False, f"You may not change {pipe or ref}, a pipe of this Pipeline."
    return True, ''


def visible_pipe(
    connector_keys: Any,
    metric_key: Any,
    location_key: Any = None,
    instance_keys: Any = None,
    viewer: Optional[Viewer] = None,
) -> Optional[mrsm.Pipe]:
    """
    The pipe with these keys on an instance the API allows (`engine.get_instance(strict=True)`),
    if `viewer` (by default the request's) may see it, else `None`. A page or a callback that
    takes a pipe's keys from the browser (a store, a component's id) asks here first. A pipe
    that is not registered has no access yet and is visible (its page offers Register).
    """
    return resolve_visible_pipe(connector_keys, metric_key, location_key, instance_keys, viewer=viewer)[0]


def resolve_visible_pipe(
    connector_keys: Any,
    metric_key: Any,
    location_key: Any = None,
    instance_keys: Any = None,
    viewer: Optional[Viewer] = None,
) -> Tuple[Optional[mrsm.Pipe], Optional[Exception]]:
    """
    `visible_pipe()` that tells why there is no pipe: `(pipe, None)` when the viewer may see it,
    `(None, None)` when they may not (or its instance is not one the API allows), and
    `(None, error)` when its instance could not be read to tell (a database outage, which names
    no pipe: the page says so instead of calling the pipe unregistered).
    """
    from meerschaum.api.dash.engine import get_instance
    viewer = viewer if viewer is not None else current_viewer()
    if not (viewer.no_auth or viewer.signed_in) or not viewer.can('pipes:read'):
        return None, None
    if str(connector_keys) == 'mrsm' and not (viewer.no_auth or viewer.is_admin):
        return None, None
    try:
        instance = get_instance((None if instance_keys in (None, '') else str(instance_keys)), strict=True)
        location_key = None if location_key in (None, 'None', '[None]') else str(location_key)
        pipe = mrsm.Pipe(str(connector_keys), str(metric_key), location_key, instance=instance)
    except Exception:
        return None, None
    if viewer.no_auth or viewer.is_admin:
        return pipe, None
    try:
        try:
            parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
        except TypeError:
            parameters = pipe.get_parameters(apply_symlinks=False) or {}
    except Exception as e:
        return None, e
    return (pipe if can_see_pipe(viewer, parameters) else None), None


def visible_pipe_from_meta(meta: Any, viewer: Optional[Viewer] = None) -> Optional[mrsm.Pipe]:
    """`visible_pipe()` of a pipe's `meta` (`connector_keys`, `metric_key`, ...) from the browser."""
    if not isinstance(meta, dict) or not meta.get('connector_keys') or not meta.get('metric_key'):
        return None
    return visible_pipe(
        meta.get('connector_keys'),
        meta.get('metric_key'),
        meta.get('location_key', None),
        meta.get('instance_keys', meta.get('instance', None)),
        viewer=viewer,
    )


def ref_is_visible(viewer: Viewer, ref: str) -> bool:
    """
    Whether the viewer may see the pipe of a `pipe_ref` (its instance is read once). A pipe that
    is not registered is visible only to administrators here: a ref names a registered pipe.
    """
    if viewer.no_auth or viewer.is_admin:
        return True
    try:
        instance_keys = str(ref).split('|', 1)[0]
        refs = visible_refs(viewer, instance_keys)
    except Exception:
        return False
    return refs is None or ref in refs


#: How a pipe the viewer may not see is named (a graph's node, a run's stage).
HIDDEN_LABEL: str = 'Hidden pipe'


def hidden_label(count: int) -> str:
    """`1 hidden pipe`, `3 hidden pipes`."""
    return f"{count} hidden pipe{'s' if count != 1 else ''}"


def partition_refs(viewer: Viewer, refs: Iterable[str]) -> Tuple[List[str], int]:
    """`(visible_refs, hidden_count)` of `pipe_ref`s (in their order, without duplicates)."""
    visible, hidden, seen = [], 0, set()
    for ref in refs:
        ref = str(ref)
        if ref in seen:
            continue
        seen.add(ref)
        if ref_is_visible(viewer, ref):
            visible.append(ref)
        else:
            hidden += 1
    return visible, hidden


def new_pipe_access(
    viewer: Viewer,
    team: Optional[str] = None,
    visibility: Optional[str] = None,
    owner: Optional[str] = None,
) -> Dict[str, Any]:
    """
    The `access` of a pipe the viewer registers from the console
    (`meerschaum.core.access.access_for_new_pipe`): its owner is the viewer (an administrator
    may name another user), its team the chosen one or the viewer's primary team.
    """
    visibility = visibility if visibility in {value for value, _ in VISIBILITIES} else 'team'
    requested: Dict[str, Any] = {'visibility': visibility, 'edit': 'team'}
    if team and not core().is_everyone(team):
        requested['team'] = str(team).strip()
    if owner and viewer.is_admin:
        requested['owner'] = str(owner).strip()
    if viewer.no_auth:
        return {**requested, 'owner': requested.get('owner'), 'team': requested.get('team')}
    primary = viewer.primary_team or EVERYONE
    access = dict(core().access_for_new_pipe(viewer.principal, requested, primary))
    if not team or core().is_everyone(team):
        ### Everyone, chosen explicitly (the primary team is only the default).
        access['team'] = None if team else access.get('team')
    return access


def assignable_teams(viewer: Viewer) -> List[str]:
    """The teams the viewer may give a pipe: Everyone and their own (every team for admins)."""
    if viewer.is_admin or viewer.no_auth:
        try:
            from meerschaum.api import get_api_connector
            names = [
                str(getattr(team, 'name', team))
                for team in mrsm.get_teams(instance=get_api_connector())
            ]
            return [EVERYONE] + sorted(name for name in names if name != EVERYONE)
        except Exception:
            return [EVERYONE]
    return [EVERYONE] + viewer.own_teams


# ------------------------------------------------------------------------------------------------
# Users' rights
# ------------------------------------------------------------------------------------------------

def user_rights(user_type: Optional[str], attributes: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    A user's rights as the Users & Teams page shows them: `{role, permissions, explicit}`:
    `role` is `admin`, a preset (`viewer`, `editor`, `operator`), or `custom`; `explicit` says
    whether the user has permissions or a role of their own (else the default role applies).
    """
    attributes = dict(attributes or {})
    if str(user_type or '').lower() == 'admin':
        return {'role': 'admin', 'permissions': set(all_permissions()), 'explicit': True}
    role = core().user_role(user_type, attributes, default_role())
    permissions = core().user_permissions(user_type, attributes, default_role())
    explicit = attributes.get('scopes') is not None or bool(attributes.get('role'))
    return {
        'role': role_name(role),
        'permissions': {str(permission) for permission in permissions},
        'explicit': explicit,
    }


def attributes_with_rights(
    attributes: Optional[Dict[str, Any]],
    role: Optional[str],
    permissions: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    The user attributes that give `role` (a preset), the explicit `permissions` (`custom`), or
    the default role (`role` is `None` or `default`): the other attributes are kept.
    """
    attributes = dict(attributes or {})
    if role in ROLE_LABELS:
        return dict(core().attributes_with_role(attributes, role))
    if role == 'custom':
        return dict(core().attributes_with_permissions(attributes, sorted(permissions or [])))
    attributes.pop('scopes', None)
    ### `None`: no role of their own (the default role). The key stays so the store writes it.
    attributes['role'] = None
    return attributes


def can_see_email(viewer: Viewer, username: Optional[str], user_teams: Optional[Iterable[Any]] = None) -> bool:
    """
    Whether the viewer may see `username`'s email address: their own, or as an administrator or
    a manager of a team the user is in (`meerschaum.core.access.can_see_email`).
    """
    if viewer.no_auth or viewer.is_admin:
        return True
    if not viewer.signed_in or not username:
        return False
    if username == viewer.username:
        return True
    pairs = []
    for item in (user_teams or []):
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            pairs.append((str(item[0]), str(item[1])))
        elif item:
            pairs.append((str(item), 'member'))
    return bool(core().can_see_email(viewer.principal, pairs, str(username)))


def filter_jobs(viewer: Viewer, jobs: Dict[str, Any]) -> Dict[str, Any]:
    """The jobs the viewer may see (`meerschaum.jobs.filter_visible_jobs`)."""
    if viewer.no_auth or viewer.is_admin:
        return dict(jobs)
    if not viewer.can('jobs:read'):
        return {}
    from meerschaum.jobs import filter_visible_jobs
    visible = filter_visible_jobs(viewer, jobs)
    if isinstance(visible, dict):
        return visible
    names = {getattr(job, 'name', job) for job in visible}
    return {name: job for name, job in jobs.items() if name in names}
