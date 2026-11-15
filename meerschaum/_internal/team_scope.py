#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The isolated team a piece of server-side work runs for (5.1), and the connector lookups it may
make.

The API server sets the team while the Python API host serves a request of a user who acts for
an isolated team (the host's request handling decides it from the request's credentials or
console session, never from anything plugin code passes), and while a Python worker runs a call
the server made for such a team (the call's `team_scope`, which only the Rust host sends). For
the duration, `get_connector()` refuses connectors outside the team's set and `get_config()` omits
them from what it returns.

This is a guard rail, not isolation. Plugin code in a shared process can read the process's
memory, its environment and every file the server's operating-system user can read; it is the
team-scoped workers and jobs (started with only the team's configuration) that keep other
connectors' credentials out of a team's processes. The scope is not reachable through the
public API: `meerschaum` offers no function that sets or clears it.

Threads started while a team is set (`threading.Thread`, `ThreadPoolExecutor.submit`) run with
the same team ([`install_thread_propagation`]).
"""

from __future__ import annotations

import contextvars
import threading
from typing import Any, Dict, FrozenSet, Iterable, Optional, Tuple

__all__ = ('TeamScope', 'current', 'check_connector', 'filter_config')

_UNSET = object()


class TeamScope:
    """
    What the work of an isolated team may look up: its connector keys and plugins. Immutable.
    """

    __slots__ = ('_team', '_connectors', '_plugins', '_configured')

    def __init__(
        self,
        team: str,
        connectors: Iterable[str] = (),
        plugins: Iterable[str] = (),
        configured: Iterable[str] = (),
    ):
        object.__setattr__(self, '_team', str(team))
        object.__setattr__(self, '_connectors', frozenset(str(k) for k in connectors))
        object.__setattr__(self, '_plugins', frozenset(str(p) for p in plugins))
        object.__setattr__(self, '_configured', frozenset(str(k) for k in configured))

    def __setattr__(self, name, value):
        raise AttributeError("A TeamScope cannot be changed.")

    def __delattr__(self, name):
        raise AttributeError("A TeamScope cannot be changed.")

    @property
    def team(self) -> str:
        return self._team

    @property
    def connectors(self) -> FrozenSet[str]:
        return self._connectors

    @property
    def plugins(self) -> FrozenSet[str]:
        return self._plugins

    def allows(self, keys: str) -> bool:
        """Whether the team may use the connector `keys` (`type:label`)."""
        keys = str(keys).strip()
        if ':' not in keys:
            keys = keys + ':main'
        return keys in self._connectors

    def may_use(self, keys: str) -> bool:
        """
        Whether plugin code of the team may build the connector `keys`: one of the team's, a
        granted data plugin (`plugin:<name>`), or a label that names no connector of the server's.
        """
        if not self._team:
            return False
        keys = str(keys).strip()
        if ':' not in keys:
            keys = keys + ':main'
        if keys in self._connectors:
            return True
        if keys.startswith('plugin:'):
            return keys[len('plugin:'):] in self._plugins
        return keys not in self._configured

    def __repr__(self) -> str:
        return f"TeamScope({self._team!r}, connectors={sorted(self._connectors)!r})"


#: The team of the current context. Unset: the open view (the process's own configuration).
_SCOPE: contextvars.ContextVar = contextvars.ContextVar('_mrsm_team_scope')

#: What a value other than a `TeamScope` (set by code other than the server's) stands for: no
#: connector at all.
_CLOSED = TeamScope('', ())


def current() -> Optional[TeamScope]:
    """
    The team the current work runs for, or `None` for the open view. A value that is not a
    `TeamScope` (anything plugin code could put there) counts as a team with no connectors.
    """
    value = _SCOPE.get(_UNSET)
    if value is _UNSET:
        return None
    if isinstance(value, TeamScope):
        return value
    return _CLOSED


def _from_wire(scope: Any) -> Optional[TeamScope]:
    """A `TeamScope` from the server's description (`team`, `connectors`, `plugins`, `configured`)."""
    if isinstance(scope, TeamScope):
        return scope
    if not isinstance(scope, dict) or not scope.get('team'):
        return None
    return TeamScope(
        scope.get('team'),
        scope.get('connectors', None) or (),
        scope.get('plugins', None) or (),
        scope.get('configured', None) or (),
    )


def _enter(scope: Any):
    """Server side only: run the current context for `scope`. Returns the token for `_exit`."""
    team_scope = _from_wire(scope)
    if team_scope is None:
        return None
    return _SCOPE.set(team_scope)


def _exit(token) -> None:
    """Server side only: undo `_enter`."""
    if token is not None:
        _SCOPE.reset(token)


def _configured_keys() -> FrozenSet[str]:
    """The connector keys this process's own configuration defines."""
    from meerschaum import _core
    connectors = _core.get_config(
        'meerschaum', 'connectors', patch=False, write_missing=False, warn=False,
    ) or {}
    keys = set()
    if isinstance(connectors, dict):
        for typ, labels in connectors.items():
            if isinstance(labels, dict):
                for label, attrs in labels.items():
                    if label != 'default' and attrs is not None:
                        keys.add(f"{typ}:{label}")
    return frozenset(keys)


def scope_for(team: Dict[str, Any]) -> TeamScope:
    """
    The `TeamScope` of a team's scope description (`team`, `connectors`, `plugins`), with this
    process's configured connector keys.
    """
    return TeamScope(
        team.get('team', ''),
        team.get('connectors', None) or (),
        team.get('plugins', None) or (),
        _configured_keys(),
    )


def refusal(keys: str) -> str:
    """The message of a refused connector (the API's wording)."""
    if str(keys).startswith('plugin:'):
        return f"The plugin '{str(keys)[len('plugin:'):]}' is not available to your teams."
    return f"The connector '{keys}' is not available to your teams."


def check_connector(type: Any, label: Any) -> None:
    """Raise `PermissionError` when the current team may not use the connector `type:label`."""
    scope = current()
    if scope is None:
        return
    keys = f"{type}:{label}"
    if scope.allows(keys):
        return
    if str(type) == 'plugin':
        if not scope.may_use(keys):
            raise PermissionError(refusal(keys))
        return
    ### A connector this process already built (with attributes in no config file) is as much
    ### the server's as a configured one.
    built = False
    try:
        from meerschaum.connectors import connectors as _built
        built = label in (_built.get(type, None) or {})
    except Exception:
        pass
    if built or not scope.may_use(keys):
        raise PermissionError(refusal(keys))


def _filter_connectors(connectors: Any, scope: TeamScope) -> Any:
    if not isinstance(connectors, dict):
        return connectors
    out = {}
    for typ, labels in connectors.items():
        if not isinstance(labels, dict):
            out[typ] = labels
            continue
        out[typ] = {
            label: attrs
            for label, attrs in labels.items()
            if label == 'default' or scope.allows(f"{typ}:{label}")
        }
    return out


def in_team_process() -> bool:
    """
    Whether this process was started for an isolated team (`MRSM_TEAM_SCOPE`): its configuration
    holds only the team's connectors, the others are `None` (not defined).
    """
    import os
    return bool(os.environ.get('MRSM_TEAM_SCOPE', None))


def _drop_undefined(connectors: Any) -> Any:
    if not isinstance(connectors, dict):
        return connectors
    return {
        typ: (
            {label: attrs for label, attrs in labels.items() if attrs is not None}
            if isinstance(labels, dict) else labels
        )
        for typ, labels in connectors.items()
    }


def filter_process_config(keys: Tuple[Any, ...], value: Any, as_tuple: bool = False) -> Any:
    """
    In a team's process, `get_config()` without the connectors its configuration leaves
    undefined (their names are not listed).
    """
    valid, result = value if as_tuple else (True, value)
    keys = tuple(str(k) for k in keys)
    if not keys and isinstance(result, dict) and isinstance(result.get('meerschaum'), dict):
        result = dict(result)
        meerschaum = dict(result['meerschaum'])
        meerschaum['connectors'] = _drop_undefined(meerschaum.get('connectors'))
        result['meerschaum'] = meerschaum
    elif keys[:1] == ('meerschaum',):
        if len(keys) == 1 and isinstance(result, dict):
            result = dict(result)
            result['connectors'] = _drop_undefined(result.get('connectors'))
        elif len(keys) == 2 and keys[1] == 'connectors':
            result = _drop_undefined(result)
        elif len(keys) == 3 and keys[1] == 'connectors':
            result = _drop_undefined({keys[2]: result}).get(keys[2])
    return (valid, result) if as_tuple else result


def filter_config(keys: Tuple[Any, ...], value: Any, as_tuple: bool = False) -> Any:
    """
    What `get_config(*keys)` returns for the current team: the connectors outside its set
    omitted (a key path into one of them is invalid).
    """
    scope = current()
    if scope is None:
        return value
    valid, result = value if as_tuple else (True, value)
    keys = tuple(str(k) for k in keys)
    if not keys:
        if isinstance(result, dict) and isinstance(result.get('meerschaum', None), dict):
            result = dict(result)
            meerschaum = dict(result['meerschaum'])
            meerschaum['connectors'] = _filter_connectors(meerschaum.get('connectors'), scope)
            result['meerschaum'] = meerschaum
    elif keys[0] == 'meerschaum':
        if len(keys) == 1:
            if isinstance(result, dict):
                result = dict(result)
                result['connectors'] = _filter_connectors(result.get('connectors'), scope)
        elif keys[1] == 'connectors':
            if len(keys) == 2:
                result = _filter_connectors(result, scope)
            elif len(keys) == 3:
                result = _filter_connectors({keys[2]: result}, scope).get(keys[2])
            elif keys[3] != 'default' and not scope.allows(f"{keys[2]}:{keys[3]}"):
                valid, result = False, None
    return (valid, result) if as_tuple else result


_installed = {'threads': False}


def install_thread_propagation() -> None:
    """
    Run the threads a team's work starts (`threading.Thread.start`,
    `concurrent.futures.ThreadPoolExecutor.submit`) with the same team. Idempotent; a thread
    started outside any team is unchanged.
    """
    if _installed['threads']:
        return
    _installed['threads'] = True
    from concurrent.futures import ThreadPoolExecutor

    original_start = threading.Thread.start

    def start(self, *args, **kwargs):
        if _SCOPE.get(_UNSET) is not _UNSET:
            context = contextvars.copy_context()
            run = self.run

            def run_in_context():
                return context.run(run)

            self.run = run_in_context
        return original_start(self, *args, **kwargs)

    threading.Thread.start = start

    original_submit = ThreadPoolExecutor.submit

    def submit(self, fn, /, *args, **kwargs):
        if _SCOPE.get(_UNSET) is not _UNSET:
            context = contextvars.copy_context()
            return original_submit(self, context.run, fn, *args, **kwargs)
        return original_submit(self, fn, *args, **kwargs)

    ThreadPoolExecutor.submit = submit


#: What a worker reports in its handshake when it can run an isolated team's code (5.1).
CAPABILITY: str = 'team_scope'


def hello_report() -> Optional[Dict[str, Any]]:
    """
    The proof a worker started for an isolated team sends in its handshake (5.1): the team it was
    started for (`MRSM_TEAM_SCOPE`), the fingerprint of the configuration it was given
    (`MRSM_TEAM_FINGERPRINT`), the configuration file it read, and the connectors its configuration
    actually defines, read back from the loaded configuration. The Rust host checks it before it
    sends the worker any call and stops a worker whose report is missing or does not match.
    `None` in a process that was not started for a team.
    """
    import os
    team = os.environ.get('MRSM_TEAM_SCOPE', '')
    if not team:
        return None
    from meerschaum import _core
    raw = _core.get_config('meerschaum', 'connectors', warn=False) or {}
    defined = sorted(
        f"{typ}:{label}"
        for typ, labels in raw.items()
        if isinstance(labels, dict)
        for label, attrs in labels.items()
        if label != 'default' and attrs
    )
    return {
        'team': team,
        'fingerprint': os.environ.get('MRSM_TEAM_FINGERPRINT', ''),
        'config_file': os.environ.get('MRSM_CONFIG_FILE', ''),
        'connectors': defined,
    }
