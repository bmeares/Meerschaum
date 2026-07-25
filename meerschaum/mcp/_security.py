#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Guardrails for the MCP tools which can reach beyond their stated scope.

Two tools need more than a scope check:

- `read_sql` runs under `sql:read`, but `SQLConnector.read()` will happily
  execute whatever it is handed. Without a statement check, a read scope grants
  arbitrary DML and DDL.
- `execute_action` runs under `actions:execute`, which reaches the `sh`, `os`,
  and `python` actions — i.e. arbitrary code execution on the API host. That is
  inherent to the scope (the REST `/actions` route has always behaved this way),
  so the default here is a denylist of the code-execution actions rather
  than a change to what the scope means.

This module also holds the per-request MCP context: the HTTP transport stashes
the authenticated principal here (via a `contextvars.ContextVar`, so it survives
the threadpool hop) so that tools can apply the same permission checks as their
REST equivalents. Over stdio no context is set and the checks pass, matching the
CLI's trust model.
"""

from __future__ import annotations

import contextvars
import re

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple

### Statements which may begin a read-only query.
_READ_ONLY_STARTS: frozenset = frozenset({
    'select',
    'with',
    'show',
    'describe',
    'desc',
    'explain',
    'values',
    'table',
})

### Words which may never appear in a query submitted to `read_sql`.
### `into` is included to catch `SELECT ... INTO <new_table>`.
_MUTATING_KEYWORDS: frozenset = frozenset({
    'insert', 'update', 'delete', 'drop', 'alter', 'create', 'truncate',
    'grant', 'revoke', 'merge', 'upsert', 'replace', 'rename', 'comment',
    'call', 'exec', 'execute', 'do', 'copy', 'into', 'outfile', 'dumpfile',
    'load', 'import', 'export', 'attach', 'detach', 'pragma', 'vacuum',
    'analyze', 'reindex', 'cluster', 'refresh', 'lock', 'unlock',
    'commit', 'rollback', 'savepoint', 'begin', 'start', 'set', 'reset',
    'prepare', 'deallocate', 'discard', 'listen', 'notify', 'shutdown',
    'kill', 'backup', 'restore', 'checkpoint', 'bulk', 'openrowset',
    'openquery', 'opendatasource',
})

### Function names which mutate state or touch the filesystem despite living
### inside a plain `SELECT`. A blocklist cannot be complete (any `VOLATILE`
### user-defined function is a write path) — the durable fix is a read-only
### database role on the connector, which is what the docs recommend.
_MUTATING_FUNCTIONS: frozenset = frozenset({
    'dblink', 'dblink_exec', 'dblink_connect', 'dblink_connect_u',
    'lo_import', 'lo_export', 'lo_unlink',
    'setval', 'nextval',
    'pg_read_file', 'pg_read_binary_file', 'pg_ls_dir', 'pg_stat_file',
    'pg_terminate_backend', 'pg_cancel_backend', 'pg_reload_conf',
    'pg_rotate_logfile', 'pg_switch_wal', 'pg_promote',
    'pg_create_restore_point', 'pg_drop_replication_slot',
    'pg_create_physical_replication_slot', 'pg_create_logical_replication_slot',
    'pg_logical_slot_get_changes', 'pg_logical_slot_peek_changes',
    'load_file', 'load_extension', 'readfile', 'writefile', 'fsync',
})

### Word prefixes which reach procedural or extension packages
### (`xp_cmdshell`, `sp_executesql`, `dbms_scheduler`, `utl_file`, ...).
_MUTATING_WORD_PREFIXES: Tuple[str, ...] = ('xp_', 'sp_', 'dbms_', 'utl_')

_WORD_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DOLLAR_QUOTE_PATTERN = re.compile(r'\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$')

### A table name, optionally schema- and database-qualified, optionally quoted:
### `pipes`, `mrsm.pipes`, `"mrsm"."pipes"`, `[dbo].[pipes]`.
_IDENTIFIER = r"""(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])"""
_BARE_IDENTIFIER_PATTERN = re.compile(
    rf'^{_IDENTIFIER}(?:\.{_IDENTIFIER}){{0,2}}$'
)


def strip_sql_noise(query: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return `(stripped, None)` with comments, string literals, and quoted
    identifiers removed, or `(None, reason)` when the query must be refused
    outright.

    A single character-by-character pass tracks quoting state so that a comment
    marker inside a string literal (or a quote inside a comment) cannot desynchronize
    the check from what the database will actually execute. Escapes are
    deliberately NOT honored inside string literals (only the standard `''`
    doubling): a scanner that consumes less than the database does can only
    reject more, never allow more.

    Refused outright:

    - MySQL executable comments (`/*! ... */`), which the server executes.
    - Unterminated strings, identifiers, dollar-quotes, or block comments.
    """
    out: List[str] = []
    i = 0
    length = len(query)

    while i < length:
        char = query[i]

        ### Single-quoted string literal ('' doubles a quote; no backslash escapes).
        if char == "'":
            i += 1
            while i < length:
                if query[i] == "'":
                    if i + 1 < length and query[i + 1] == "'":
                        i += 2
                        continue
                    break
                i += 1
            else:
                return None, "Unterminated string literal."
            if i >= length:
                return None, "Unterminated string literal."
            out.append("''")
            i += 1
            continue

        ### Quoted identifiers: "..." (standard), `...` (MySQL), [...] (MSSQL).
        if char in ('"', '`', '['):
            closing = ']' if char == '[' else char
            end = query.find(closing, i + 1)
            if end == -1:
                return None, "Unterminated quoted identifier."
            out.append(' ')
            i = end + 1
            continue

        ### PostgreSQL dollar-quoted string: $$...$$ or $tag$...$tag$.
        if char == '$':
            match = _DOLLAR_QUOTE_PATTERN.match(query, i)
            if match:
                tag = match.group(0)
                end = query.find(tag, match.end())
                if end == -1:
                    return None, "Unterminated dollar-quoted string."
                out.append("''")
                i = end + len(tag)
                continue
            out.append(char)
            i += 1
            continue

        ### Line comments: -- and # run to end of line.
        if char == '#' or query.startswith('--', i):
            newline = query.find('\n', i)
            out.append(' ')
            if newline == -1:
                break
            i = newline
            continue

        ### Block comments. `/*!` is executed by MySQL, so refuse it.
        if query.startswith('/*', i):
            if query.startswith('/*!', i):
                return None, (
                    "Refusing an executable comment (`/*! ... */`), "
                    "which MySQL runs as SQL."
                )
            end = query.find('*/', i + 2)
            if end == -1:
                return None, "Unterminated block comment."
            out.append(' ')
            i = end + 2
            continue

        out.append(char)
        i += 1

    return ''.join(out), None


def is_read_only_query(query: str) -> Tuple[bool, str]:
    """
    Return whether `query` is a single read-only SQL statement.

    A bare identifier (a table name) is accepted, because `SQLConnector.read()`
    accepts a table name in place of a query.

    Returns
    -------
    A `SuccessTuple`-shaped result: `(True, "Success")` when the query may be
    executed, otherwise `(False, reason)`.
    """
    if not query or not query.strip():
        return False, "Refusing to execute an empty query."

    ### A bare table name is a valid input to `SQLConnector.read()`. Test this
    ### against the original text, before quoted identifiers are stripped.
    if _BARE_IDENTIFIER_PATTERN.match(query.strip().rstrip(';').strip()):
        return True, "Success"

    stripped, refusal = strip_sql_noise(query)
    if refusal:
        return False, f"Refusing to execute the query: {refusal}"
    stripped = stripped.strip()

    ### Reject stacked statements. A single trailing semicolon is fine.
    statements = [part for part in stripped.split(';') if part.strip()]
    if len(statements) > 1:
        return False, (
            "Refusing to execute multiple statements. "
            "Submit one read-only query at a time."
        )

    if not statements:
        return False, "Refusing to execute an empty query."

    words = [word.lower() for word in _WORD_PATTERN.findall(statements[0])]
    if not words:
        return False, "Could not parse a statement from the query."

    if words[0] not in _READ_ONLY_STARTS:
        return False, (
            f"Refusing to execute a '{words[0].upper()}' statement. "
            f"`read_sql` only accepts read-only queries "
            f"({', '.join(sorted(_READ_ONLY_STARTS))})."
        )

    for word in words:
        if word in _MUTATING_KEYWORDS or word in _MUTATING_FUNCTIONS:
            return False, (
                f"Refusing to execute a query containing '{word.upper()}'. "
                "`read_sql` only accepts read-only queries; use a dedicated "
                "write tool or the `execute_action` tool instead."
            )
        if word.startswith(_MUTATING_WORD_PREFIXES):
            return False, (
                f"Refusing to execute a query containing '{word}': "
                "procedural packages are not reachable from `read_sql`."
            )

    return True, "Success"


### ponytail: keyword-level statement check, not a parser. It blocks the
### realistic escalation paths from `sql:read` (stacked statements, DML,
### DDL, `SELECT ... INTO`, executable comments, the known mutating built-ins)
### but a VOLATILE user-defined function is inherently invisible to it. The
### durable fix is a read-only database role on the connector, which is what
### the docs recommend for any deployment exposing `read_sql` to untrusted
### callers. Swap this for a real parser (or a `SET TRANSACTION READ ONLY`
### wrapper per flavor) if that ever stops being enough.


### Per-request context set by the HTTP transport (`meerschaum.api.routes._mcp`)
### so tools can enforce the API server's permission model. `None` (the default,
### and always the case over stdio) means full local trust.
_MCP_CONTEXT: contextvars.ContextVar = contextvars.ContextVar('mcp_context', default=None)


def set_mcp_context(context: Optional[Dict[str, Any]]) -> contextvars.Token:
    """
    Set the per-request MCP context and return the token for `reset_mcp_context()`.
    """
    return _MCP_CONTEXT.set(context)


def reset_mcp_context(token: contextvars.Token) -> None:
    """
    Restore the MCP context to its state before `set_mcp_context()`.
    """
    _MCP_CONTEXT.reset(token)


def get_mcp_context() -> Optional[Dict[str, Any]]:
    """
    Return the per-request MCP context (`None` outside the HTTP transport).
    """
    return _MCP_CONTEXT.get()


def check_instance_keys(instance_keys: Optional[str]) -> Tuple[bool, str]:
    """
    Return whether the caller may reach `instance_keys`.

    Over HTTP this applies the API server's instance permissions
    (`allow_multiple_instances` and `allowed_instance_keys`) by resolving the
    connector through `meerschaum.api.get_api_connector()`. Over stdio there is
    no restriction.
    """
    context = get_mcp_context()
    if not context or not context.get('api'):
        return True, "Success"

    from meerschaum.api import get_api_connector
    try:
        get_api_connector(instance_keys or None)
    except Exception as e:
        return False, str(e)

    return True, "Success"


def check_action_execution_allowed() -> Tuple[bool, str]:
    """
    Return whether the current principal may execute actions at all,
    mirroring the REST `/actions` route's `is_user_allowed_to_execute()` check.
    """
    context = get_mcp_context()
    if not context or not context.get('api'):
        return True, "Success"

    from meerschaum.core.User import is_user_allowed_to_execute
    return is_user_allowed_to_execute(context.get('user'))


def get_action_denylist() -> List[str]:
    """
    Return the action names `execute_action` refuses to run.
    """
    return list(
        mrsm.get_config('api', 'mcp', 'actions', 'denylist', warn=False)
        or []
    )


def get_action_allowlist() -> List[str]:
    """
    Return the only action names `execute_action` may run.

    An empty list means every action not in the denylist is permitted.
    """
    return list(
        mrsm.get_config('api', 'mcp', 'actions', 'allowlist', warn=False)
        or []
    )


def is_action_permitted(action_name: str) -> Tuple[bool, str]:
    """
    Return whether `execute_action` may run `action_name`.
    """
    allowlist = get_action_allowlist()
    if allowlist and action_name not in allowlist:
        return False, (
            f"Action '{action_name}' is not in this server's MCP action allowlist. "
            f"Permitted actions: {', '.join(sorted(allowlist))}."
        )

    if action_name in get_action_denylist():
        return False, (
            f"Action '{action_name}' is blocked by this server's MCP action denylist "
            "because it executes arbitrary code on the API host. "
            "Set `api:mcp:actions:denylist` in the Meerschaum config to change this."
        )

    return True, "Success"


def check_action_chain(words: List[str]) -> Tuple[bool, str]:
    """
    Check every word of a subaction chain against the denylist, so a denylisted
    action cannot be smuggled in through a wrapper (e.g. `start job` with
    `{"action": ["sh", ...]}`, or `start jobs sh ...`).

    Denylist-only on purpose: subaction words (e.g. the 'pipes' in 'sync pipes')
    must not be required to appear in the allowlist.
    """
    denylist = get_action_denylist()
    for word in words:
        word = str(word)
        if word in denylist:
            return False, (
                f"Action '{word}' is blocked by this server's MCP action denylist "
                "and may not be run through another action either."
            )

    return True, "Success"
