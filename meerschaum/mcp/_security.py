#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Guardrails for the MCP tools which can reach beyond their stated scope.

Two tools need more than a scope check:

- `read_sql` runs under `connectors:read`, but `SQLConnector.read()` will happily
  execute whatever it is handed. Without a statement check, a read scope grants
  arbitrary DML and DDL.
- `execute_action` runs under `actions:execute`, which reaches the `sh`, `os`,
  and `python` actions — i.e. arbitrary code execution on the API host. That is
  inherent to the scope (the REST `/actions` route has always behaved this way),
  so the default here is a denylist of the three code-execution actions rather
  than a change to what the scope means.
"""

from __future__ import annotations

import re

import meerschaum as mrsm
from meerschaum.utils.typing import List, Tuple

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
})

_COMMENT_PATTERN = re.compile(r'/\*.*?\*/', re.DOTALL)
_LINE_COMMENT_PATTERN = re.compile(r'(--|#)[^\n]*')
_WORD_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_STRING_PATTERN = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")

### A table name, optionally schema- and database-qualified, optionally quoted:
### `pipes`, `mrsm.pipes`, `"mrsm"."pipes"`, `[dbo].[pipes]`.
_IDENTIFIER = r"""(?:[A-Za-z_][A-Za-z0-9_$]*|"[^"]+"|`[^`]+`|\[[^\]]+\])"""
_BARE_IDENTIFIER_PATTERN = re.compile(
    rf'^{_IDENTIFIER}(?:\.{_IDENTIFIER}){{0,2}}$'
)


def strip_sql_noise(query: str) -> str:
    """
    Return `query` with comments and string literals removed.

    String literals are stripped so that a keyword inside a literal (e.g.
    `WHERE note = 'please delete'`) does not trip the keyword check.
    """
    stripped = _COMMENT_PATTERN.sub(' ', query)
    stripped = _LINE_COMMENT_PATTERN.sub(' ', stripped)
    stripped = _STRING_PATTERN.sub("''", stripped)
    return stripped


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

    stripped = strip_sql_noise(query).strip()

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
        if word in _MUTATING_KEYWORDS:
            return False, (
                f"Refusing to execute a query containing '{word.upper()}'. "
                "`read_sql` only accepts read-only queries; use a dedicated "
                "write tool or the `execute_action` tool instead."
            )

    return True, "Success"


### ponytail: keyword-level statement check, not a parser. It blocks the
### realistic escalation paths from `connectors:read` (stacked statements, DML,
### DDL, `SELECT ... INTO`, `COPY ... TO PROGRAM`) but a sufficiently exotic
### flavor-specific construct could still slip through. The durable fix is a
### read-only database role on the connector, which is what the docs recommend
### for any deployment exposing `read_sql` to untrusted callers. Swap this for a
### real parser (or a `SET TRANSACTION READ ONLY` wrapper per flavor) if that
### ever stops being enough.


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
