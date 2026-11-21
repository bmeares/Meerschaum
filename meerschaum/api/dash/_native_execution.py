"""Fresh native execution projection for one real console session.

This file grants no transport/backend authority. Only a fixed authenticated Main endpoint
may restore an action scope; normal Core object/Team rules remain unchanged.
"""

from dataclasses import dataclass, replace
import json
import uuid


@dataclass(frozen=True)
class NativeExecution:
    known: bool = False
    actions_execute: bool = False
    jobs_execute: bool = False
    jobs_write: bool = False
    api_terminals: bool = False
    terminal_exec: bool = False


def _binding(session_id):
    from meerschaum.api.dash.sessions import get_session_data
    data = get_session_data(session_id, use_cache=False)
    if not isinstance(data, dict):
        return None
    username, credential = data.get('username'), data.get('credential')
    if not isinstance(username, str) or not username or not isinstance(credential, str) or not credential:
        return None
    # Freeze values, not the mutable store/cache dictionary.
    return username, credential


def _exchange(session_id, path, payload=None):
    import urllib.parse
    import urllib.request
    from meerschaum.api.dash.execute import server_url
    if path == '/execute/capabilities' and payload is None:
        limit, timeout = 4096, 3
    elif path == '/mcp' and payload is not None:
        limit, timeout = 65536, 70
    else:
        raise ValueError('Only fixed native console surfaces are supported.')
    base = server_url()
    if not isinstance(base, str):
        raise ValueError('Native execution projection is unavailable.')
    url = urllib.parse.urlsplit(base)
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username
            or url.password or url.path not in ('', '/') or url.query or url.fragment):
        raise ValueError('Invalid trusted Main origin.')

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    headers = {'accept': 'application/json', 'cookie': 'mrsm-session-id=' + session_id}
    # Preserve an explicitly presented bearer ceiling. It may refuse or narrow this
    # same-user console request; the cookie must never replace it with broader scopes.
    from flask import has_request_context, request as browser_request
    if has_request_context() and 'Authorization' in browser_request.headers:
        headers['authorization'] = browser_request.headers['Authorization']
    data = None
    if payload is not None:
        data = json.dumps(payload, separators=(',', ':')).encode()
        if len(data) > 16384:
            raise ValueError('The typed pipe request exceeds its bound.')
        headers['content-type'] = 'application/json'
    request = urllib.request.Request(
        base.rstrip('/') + path, data=data, headers=headers,
        method='GET' if payload is None else 'POST',
    )
    with opener.open(request, timeout=timeout) as response:
        if response.status != 200:
            raise ValueError('Native execution projection was refused.')
        body = response.read(limit + 1)
    if len(body) > limit:
        raise ValueError('Native execution projection exceeds its bound.')
    return json.loads(body.decode('utf-8'))


def _read(session_id):
    return _exchange(session_id, '/execute/capabilities')


def _validate(value, viewer):
    if (not isinstance(value, dict)
            or set(value) != {'schema', 'username', 'source', 'permissions', 'capabilities', 'policy'}
            or type(value['schema']) is not int or value['schema'] != 1
            or value['username'] != viewer.username):
        raise ValueError('Invalid native execution projection.')
    groups = (
        ('permissions', {'actions_execute', 'jobs_execute', 'jobs_write', 'webterm_use'}),
        ('capabilities', {'tracked_api', 'api_terminals'}),
        ('policy', {'non_admin_actions'}),
    )
    for name, keys in groups:
        if (not isinstance(value[name], dict) or set(value[name]) != keys
                or any(type(flag) is not bool for flag in value[name].values())):
            raise ValueError('Invalid native execution flags.')
    source = value['source']
    if (not isinstance(source, dict)
            or set(source) != {'primary_team', 'team_id', 'isolated', 'runs_code'}
            or source['primary_team'] != getattr(viewer, 'real_primary_team', viewer.primary_team)
            or source['isolated'] is not True or source['runs_code'] is not False
            or not isinstance(source['team_id'], str)):
        raise ValueError('The native execution source changed.')
    identity = uuid.UUID(source['team_id'])
    if identity.int == 0 or str(identity) != source['team_id']:
        raise ValueError('Invalid registered native source.')
    # Current public API terminal admission remains closed. Lifting it needs its own
    # reviewed UI/runtime contract; an action permission never selects a terminal.
    if value['capabilities']['api_terminals'] is not False:
        raise ValueError('This projection does not enable API terminals.')
    return value


def _principal(viewer):
    from meerschaum.api.dash.access import _principal as object_principal
    return object_principal(viewer)


def _replace(viewer, permissions, execution):
    projected = replace(viewer, permissions=frozenset(permissions), native_execution=execution)
    projected.principal = _principal(projected)
    return projected


def api_only(viewer):
    return bool(viewer.signed_in and not viewer.no_auth and not viewer.is_admin
                and viewer.is_isolated and not viewer.can_write_plugins)


def _permissions(viewer):
    permissions = frozenset(viewer.permissions)
    if '*' in permissions:
        from meerschaum.api.dash.access import all_permissions
        permissions = (permissions - {'*'}) | frozenset(all_permissions())
    return permissions


def project(viewer, session_id):
    """Apply after each base Viewer lookup; never write this overlay into the base cache."""
    if not api_only(viewer):
        return viewer
    # A stale base cache or formerly positive overlay must not retain a launch scope or
    # its old Core Principal when Main cannot establish this exact source now.
    permissions = _permissions(viewer) - {
        'actions:execute', 'webterm:use', 'jobs:execute', 'jobs:write',
    }
    refused = _replace(viewer, permissions, NativeExecution())
    try:
        if not isinstance(session_id, str) or str(uuid.UUID(session_id)) != session_id:
            return refused
        before = _binding(session_id)
        if before is None or before[0] != viewer.username:
            return refused
        native = _validate(_read(session_id), viewer)
        if _binding(session_id) != before:
            return refused
        flags, capability = native['permissions'], native['capabilities']
        if not capability['tracked_api'] or not native['policy']['non_admin_actions']:
            return refused
        permissions = _permissions(viewer) - {'actions:execute', 'webterm:use'}
        ### Acting as a team (the navbar's Team switcher) is a console view: Main runs actions
        ### under the account's real identity, so they stay withheld until All teams.
        allowed = (capability['tracked_api'] and native['policy']['non_admin_actions']
                   and flags['actions_execute'] and not getattr(viewer, 'acting_team', None))
        if allowed:
            permissions |= {'actions:execute'}
        # A current native refusal removes these scopes; this overlay never adds job scopes.
        for permission, flag in [('jobs:execute', 'jobs_execute'), ('jobs:write', 'jobs_write')]:
            if not flags[flag]:
                permissions -= {permission}
        execution = NativeExecution(
            known=True,
            actions_execute=allowed,
            jobs_execute='jobs:execute' in permissions,
            jobs_write='jobs:write' in permissions,
        )
        return _replace(viewer, permissions, execution)
    except Exception:
        # No raw response/credential diagnostics; no legacy, local action or terminal fallback.
        return refused


def pipe_action(viewer, session_id, meta, action, begin=None, end=None):
    """Call only Main's built-in typed Sync/Verify; never construct a command or token."""
    unavailable = 'The native pipe result is unavailable. Check the pipe state before retrying.'
    try:
        if not api_only(viewer) or not viewer.can('pipes:write') or action not in ('sync', 'verify'):
            return False, 'This pipe operation is unavailable to this console source.'
        if not isinstance(session_id, str) or str(uuid.UUID(session_id)) != session_id:
            return False, 'A current console session is required.'
        before = _binding(session_id)
        if before is None or before[0] != viewer.username:
            return False, 'A current console session is required.'
        # This proves the same actual console subject and constructor-owned tracked
        # bridge, including any explicit bearer ceiling. It requires no action/job
        # permission: the native MCP operation independently requires pipes:write.
        native = _validate(_read(session_id), viewer)
        if not native['capabilities']['tracked_api']:
            return False, 'The native typed pipe service is unavailable.'
        if _binding(session_id) != before:
            return False, unavailable
        if not isinstance(meta, dict):
            raise ValueError('Invalid pipe identity.')
        arguments = {}
        for key in ('instance_keys', 'connector_keys', 'metric_key', 'location_key'):
            value = meta.get(key)
            if value is None and key == 'location_key':
                continue
            if (not isinstance(value, str) or not value or len(value) > 2048
                    or any(ord(character) < 32 for character in value)):
                raise ValueError('Invalid pipe identity.')
            arguments[key] = value
        for key, bound in [('begin', begin), ('end', end)]:
            if bound is None or bound == '':
                continue
            if (not isinstance(bound, str) or len(bound) > 128
                    or any(ord(character) < 32 for character in bound)):
                raise ValueError('Invalid typed pipe bound.')
            arguments[key] = bound
        request_id = uuid.uuid4().hex
        payload = {'jsonrpc': '2.0', 'id': request_id, 'method': 'tools/call',
                   'params': {'name': 'sync_pipe' if action == 'sync' else 'verify_pipe',
                              'arguments': arguments}}
        body = _exchange(session_id, '/mcp', payload)
        if _binding(session_id) != before:
            return False, unavailable
        if (not isinstance(body, dict) or body.get('jsonrpc') != '2.0'
                or body.get('id') != request_id or 'error' in body):
            return False, 'The native pipe request was refused.'
        result = body.get('result')
        if not isinstance(result, dict) or result.get('isError') is not False:
            return False, 'The native pipe request was refused.'
        content = result.get('content')
        if (not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict)
                or content[0].get('type') != 'text' or not isinstance(content[0].get('text'), str)):
            raise ValueError('Invalid typed pipe result.')
        answer = json.loads(content[0]['text'])
        if (not isinstance(answer, dict) or set(answer) != {'success', 'message'}
                or type(answer['success']) is not bool or not isinstance(answer['message'], str)
                or len(answer['message']) > 8192):
            raise ValueError('Invalid typed pipe result.')
        return answer['success'], answer['message']
    except Exception:
        # HTTP interruption may lose an already completed result. Never retry a mutation,
        # mint a broader JWT, type into a terminal, or run the action inside this host.
        return False, unavailable
