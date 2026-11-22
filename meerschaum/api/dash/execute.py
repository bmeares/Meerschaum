#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Compose console commands and authorize them through the native server. Exec types a command
exactly once into the existing terminal using the readiness/acknowledgment protocol. Pipe
management and Pipeline runs use `/execute` workers and follow their server log streams.
The host never runs an action locally. Server policy remains authoritative for authorization.
"""

from __future__ import annotations

import shlex
import time

from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple

#: Actions the dashboard never offers (they are not for a remote console). This is the built-in
#: copy of the server's `CONSOLE_OMIT_ACTIONS` (`crates/meerschaum-server/src/execute_policy.rs`,
#: published at `GET /execute/policy`); it is used when the server cannot be asked, and a test of
#: the server crate pins the two lists to each other.
OMIT_ACTIONS = frozenset({
    'api',
    'sh',
    'os',
    'sql',
    'stack',
    'reload',
    'repo',
    'instance',
})

#: How long the server's policy is kept (seconds), and how long the built-in copy stands in for
#: a policy that could not be fetched before asking again.
POLICY_TTL_SECONDS: float = 300.0
POLICY_RETRY_SECONDS: float = 30.0

_policy_cache: Dict[str, Any] = {'policy': None, 'expires': 0.0}


def _clean(values: Optional[List[Any]]) -> List[Optional[str]]:
    return [value for value in (values or []) if value is None or isinstance(value, str)]


def build_sysargs(
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[Optional[str]]] = None,
    tags: Optional[List[str]] = None,
    action: Optional[str] = None,
    subaction: Optional[str] = None,
    subaction_text: Optional[str] = None,
    flags: Optional[List[str]] = None,
    input_flags: Optional[List[Optional[str]]] = None,
    input_flags_texts: Optional[List[Optional[str]]] = None,
    instance: Optional[str] = None,
    allowed_flags: Optional[List[str]] = None,
) -> Tuple[Optional[List[str]], str]:
    """
    Return `(sysargs, message)` for the dashboard's selections: the line v4 typed into the
    terminal, as an argument list. `sysargs` is `None` (and `message` says why) if the selection is
    not something the dashboard may run.
    """
    action = (action or '').strip()
    if not action:
        return None, "Choose an action."
    if action in omit_actions():
        return None, f"The action '{action}' cannot be run from the dashboard."

    args: List[str] = [action]
    for text in (subaction, subaction_text):
        text = (text or '').strip()
        if not text:
            continue
        try:
            args += shlex.split(text)
        except ValueError as e:
            return None, f"Invalid arguments '{text}': {e}"

    for flag, values in (
        ('-c', _clean(connector_keys)),
        ('-m', _clean(metric_keys)),
        ('-l', _clean(location_keys)),
        ('-t', _clean(tags)),
    ):
        values = [('None' if value is None else value) for value in values]
        if values:
            args += [flag] + values

    allowed = set(allowed_flags) if allowed_flags is not None else None
    if instance:
        args += ['-i', str(instance)]
    for flag in _clean(flags):
        if not flag:
            continue
        if allowed is not None and flag not in allowed:
            return None, f"The flag '{flag}' cannot be used from the dashboard."
        args.append(flag)

    for flag, value in zip(input_flags or [], input_flags_texts or []):
        if not flag or not value:
            continue
        if not isinstance(flag, str) or not isinstance(value, str):
            continue
        if allowed is not None and flag not in allowed:
            return None, f"The flag '{flag}' cannot be used from the dashboard."
        args += [flag, value]
    return args, ''


#: The actions of a pipe card's "Manage" menu that ask for confirmation first (v4 asked in the terminal).
DESTRUCTIVE_PIPE_ACTIONS = frozenset({'delete', 'drop', 'clear'})
PIPE_ACTIONS = frozenset({'delete', 'drop', 'clear', 'verify', 'sync'})


def build_pipe_sysargs(
    meta: Dict[str, Any],
    action: str,
    confirmed: bool = False,
) -> Tuple[Optional[List[str]], str]:
    """
    Return `(sysargs, message)` for an action of a pipe card's "Manage" menu: `<action> pipes`
    on that pipe. A destructive action runs only `confirmed` (with `--yes`: the job cannot ask).
    """
    if action not in PIPE_ACTIONS:
        return None, f"The action '{action}' cannot be run from a pipe card."
    if action in DESTRUCTIVE_PIPE_ACTIONS and not confirmed:
        return None, f"Confirm to {action} this pipe."
    try:
        args = [
            action, 'pipes',
            '-c', str(meta['connector_keys']),
            '-m', str(meta['metric_key']),
            '-l', str(meta.get('location_key') or 'None'),
            '-i', str(meta['instance_keys']),
        ]
    except (KeyError, TypeError):
        return None, "Invalid pipe."
    if action in DESTRUCTIVE_PIPE_ACTIONS:
        args.append('--yes')
    return args, ''


def _fetch_json(path: str, username: Optional[str] = None, timeout: float = 5.0) -> Any:
    """
    `GET` `path` of the server (with a bearer for `username` if there is one) and return its JSON.
    """
    from meerschaum.api.host._server_client import (
        UserBearerAuth, get_client,
    )
    client = get_client()
    if not client.available:
        raise ValueError("No server.")
    return client.request_json(
        'GET',
        path,
        auth=UserBearerAuth(username),
        timeout=timeout,
    )


def get_policy() -> Dict[str, Any]:
    """
    Return the server's execute policy (`GET /execute/policy`: `omit_actions`, `omit_flags`, ...),
    cached for a few minutes. If the server cannot be asked the built-in lists stand in, and the
    server is asked again after a short while.
    """
    now = time.monotonic()
    if _policy_cache['policy'] is not None and now < _policy_cache['expires']:
        return _policy_cache['policy']
    try:
        policy = _fetch_json('/execute/policy')
        if not isinstance(policy, dict) or not isinstance(policy.get('omit_actions'), list):
            raise ValueError("Unexpected policy.")
        ttl = POLICY_TTL_SECONDS
    except Exception:
        policy = {'omit_actions': sorted(OMIT_ACTIONS), 'omit_flags': []}
        ttl = POLICY_RETRY_SECONDS
    _policy_cache.update(policy=policy, expires=now + ttl)
    return policy


def omit_actions() -> frozenset:
    """
    The actions the dashboard does not offer: the server's list, or the built-in one.
    """
    return frozenset(get_policy().get('omit_actions') or OMIT_ACTIONS)


def omit_flags() -> frozenset:
    """
    The flags the dashboard does not offer, beyond the ones its own flag lists leave out.
    """
    return frozenset(get_policy().get('omit_flags') or [])


def post_execute(sysargs: List[str], username: Optional[str], metadata=None) -> Tuple[Dict[str, Any], str]:
    """
    `POST /execute` to the server. Return `(response, message)`; `response` is
    `{'success', 'msg', 'job', 'stream'}`-like (the server answers `{"success", "message", "job",
    "sysargs", "stream", "logs"}`; `stream` is `None` when it does not serve the log stream). When
    the server cannot be asked the response is a failure that says why (there is no local
    fallback).
    """
    from meerschaum.api.host._server_client import (
        ServerAuthError, ServerHTTPError, UserBearerAuth, get_client,
    )
    client = get_client()
    if not client.available:
        return {
            'success': False,
            'msg': "The console has no server to run actions through (`server_url` is not set).",
        }, ''
    payload = {
        'sysargs': list(sysargs),
        **{
            key: value for key, value in (metadata or {}).items()
            if key in ('pipeline', 'run_id', 'parent_job')
        },
    }
    try:
        body = client.request_json(
            'POST',
            '/execute',
            auth=UserBearerAuth(username),
            json_body=payload,
            timeout=30,
        )
    except ServerAuthError as e:
        return {'success': False, 'msg': f"Could not authorize the action: {e}"}, ''
    except ServerHTTPError as e:
        if e.status in (404, 405, 501):
            return {
                'success': False,
                'msg': (
                    f"The server does not run actions from the console ({e.status}: "
                    "`/execute` is not available; check `api:permissions:routes:allowlist`)."
                ),
            }, ''
        return {'success': False, 'msg': f"The server refused the action: {e.message}"}, ''
    except Exception as e:
        return {'success': False, 'msg': f"Could not reach the server: {e}"}, ''

    ### A server before 5.0.0rc15 refused with a `SuccessTuple` (`[false, "message"]`).
    if isinstance(body, list) and len(body) == 2:
        return {'success': bool(body[0]), 'msg': str(body[1])}, ''
    if not isinstance(body, dict):
        return {'success': False, 'msg': "The server answered with something unexpected."}, ''
    if 'msg' not in body and 'message' in body:
        body = {**body, 'msg': body['message']}
    return body, ''


def run_action(sysargs: List[str], username: Optional[str], metadata=None) -> Dict[str, Any]:
    """
    Run an action through the server's `/execute` (never in this process).

    Returns
    -------
    `{'success', 'msg', 'job', 'stream'}`; `stream` is the SSE path of the job's log. A failure
    carries the server's message (or why the server could not be asked).
    """
    response, _ = post_execute(sysargs, username, metadata=metadata)
    success = bool(response.get('success', False))
    job = response.get('job', None)
    ### The server names the log stream only when it serves it.
    stream = (
        response.get('stream', None)
        if 'stream' in response
        else (f"/logs/{job}/stream" if job else None)
    )
    return {
        'success': success and bool(job),
        'msg': response.get('msg', response.get('message', '')),
        'job': job,
        'stream': stream,
    }


def execution_request(path, username, method='GET', body=None):
    """
    Ask the authenticated server for retained summaries, command validation or worker promotion.
    A refusal of `/execute/validate` is `[False, "The server refused this command: <reason>"]`;
    any other error raises `ValueError(reason)`.
    """
    from meerschaum.api.host._server_client import (
        ServerHTTPError, UserBearerAuth, get_client,
    )
    client = get_client()
    if not client.available:
        return None
    try:
        return client.request_json(
            method,
            path,
            auth=UserBearerAuth(username),
            json_body=body,
            timeout=30,
            unwrap_envelope=True,
        )
    except ServerHTTPError as exc:
        if path.startswith('/execute/validate') and exc.status in (400, 401, 403):
            ### The wording the console showed for a refused command before rc15.
            return [False, f"The server refused this command: {exc.message}"]
        raise ValueError(exc.message) from exc
